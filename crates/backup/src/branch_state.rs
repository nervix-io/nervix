//! Public deduplicator keyspace and window processor state records.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Archive-owned descriptors of one branch's deduplicator keyspace and window state,
//!   and the bound of the Arrow IPC groups beside them; the groups stay separate sections.
//! - **Depends on.** The archive record envelope and vocabulary identities.
//! - **Must not know.** Native checkpoint encodings, runtime ownership or restore placement.

use error_stack::Report;
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelName, RemoteRuntimeRecordMetadata, SchemaFingerprint,
    Timestamp, WindowModelDigest,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    ArchiveReadError, ArchiveRecord, ArchiveWriteError, RecordKind, StateField,
    section::{decode_record, encode_record},
    state::validate_branch,
};

/// The largest Arrow IPC group of deduplicator keys, window input rows or window arguments a
/// reader accepts. Conversion holds one group at a time.
pub const BRANCH_STATE_GROUP_BYTES: u64 = 8 * 1024 * 1024;

/// The last column of every deduplicator key group: when each key was seen, in UTC nanoseconds.
pub const DEDUPLICATOR_SEEN_AT_COLUMN: &str = "seen_at";

/// The name of the key column holding the value of `DEDUPLICATE ON` expression `index`.
pub fn deduplicator_key_column(index: usize) -> String {
    format!("key_{index}")
}

/// One branch's deduplicator keyspace at the revision its owner published.
///
/// Each group is an Arrow IPC stream with one column per `DEDUPLICATE ON` expression, `key_0`
/// onwards, typed as the expression produces it, followed by the `seen_at` timestamp. Keys are in
/// the order they expire, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeduplicatorStateDescriptor {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    pub branch_fingerprint: Option<BranchKeyFingerprint>,
    pub branch: Option<Vec<StateField>>,
    pub revision: u64,
    pub keys: u64,
    pub groups: u32,
}

/// One branch's window: everything its retained rows cannot carry themselves.
///
/// Each group holds the same consecutive retained rows twice, oldest first: `input.arrow` under the
/// window's input relay schema and `arguments.arrow` with the aggregate argument columns the rows
/// were admitted with. Restoring re-admits the rows in order and then applies the delayed
/// histogram removals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowStateDescriptor {
    pub domain: DomainName,
    pub entity: ModelName,
    pub schema: SchemaFingerprint,
    /// The window processor model the state was built under.
    pub model: WindowModelDigest,
    pub branch_fingerprint: Option<BranchKeyFingerprint>,
    pub branch: Option<Vec<StateField>>,
    pub revision: u64,
    /// The branch lifetime the window belongs to; zero is never a lifetime.
    pub incarnation: u64,
    /// The admission sequence of the oldest retained row; absent when the window retains none.
    pub first_sequence: Option<u64>,
    pub next_sequence: u64,
    /// Each retained row's ingestion watermarks, oldest first.
    pub rows: Vec<RemoteRuntimeRecordMetadata>,
    pub groups: u32,
    /// What each aggregate structure keeps beyond the retained rows, in demand order.
    pub accumulators: Vec<WindowAccumulatorRecord>,
}

/// What one aggregate structure of a window keeps beyond the rows the window retains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowAccumulatorRecord {
    /// Rebuilt entirely from the retained rows.
    Retained,
    /// A linear histogram also counts stepped rows until their delay expires.
    LinearHistogram {
        delayed_removals: Vec<DelayedHistogramRemoval>,
    },
}

/// One stepped row a linear histogram still counts, and when it stops counting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayedHistogramRemoval {
    pub expires_at: Timestamp,
    pub bucket: u64,
}

#[derive(Archive, Serialize, Deserialize)]
struct DeduplicatorDescriptorWire {
    domain: String,
    entity: String,
    schema: [u8; 32],
    branch_fingerprint: Option<[u8; 32]>,
    branch: Option<Vec<StateField>>,
    revision: u64,
    keys: u64,
    groups: u32,
}

#[derive(Archive, Serialize, Deserialize)]
struct WindowDescriptorWire {
    domain: String,
    entity: String,
    schema: [u8; 32],
    model: [u8; 32],
    branch_fingerprint: Option<[u8; 32]>,
    branch: Option<Vec<StateField>>,
    revision: u64,
    incarnation: u64,
    first_sequence: Option<u64>,
    next_sequence: u64,
    rows: Vec<WindowRowWire>,
    groups: u32,
    accumulators: Vec<WindowAccumulatorWire>,
}

#[derive(Archive, Serialize, Deserialize)]
struct WindowRowWire {
    low_watermark_unix_nanos: i64,
    high_watermark_unix_nanos: i64,
}

#[derive(Archive, Serialize, Deserialize)]
enum WindowAccumulatorWire {
    Retained,
    LinearHistogram {
        delayed_removals: Vec<DelayedRemovalWire>,
    },
}

#[derive(Archive, Serialize, Deserialize)]
struct DelayedRemovalWire {
    expires_at_unix_nanos: i64,
    bucket: u64,
}

impl ArchiveRecord for DeduplicatorStateDescriptor {
    const KIND: RecordKind = RecordKind::DeduplicatorStateDescriptor;
    const VERSION: u16 = 1;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        encode_record(
            Self::KIND,
            Self::VERSION,
            &DeduplicatorDescriptorWire {
                domain: self.domain.as_str().to_string(),
                entity: self.entity.as_str().to_string(),
                schema: *self.schema.as_digest(),
                branch_fingerprint: self
                    .branch_fingerprint
                    .as_ref()
                    .map(|key| *key.fingerprint()),
                branch: self.branch.clone(),
                revision: self.revision,
                keys: self.keys,
                groups: self.groups,
            },
        )
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: DeduplicatorDescriptorWire =
            decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        validate_branch(path, &wire.branch)?;
        if wire.branch.is_some() != wire.branch_fingerprint.is_some() {
            return Err(invalid(path, "deduplicator branch fingerprint"));
        }
        validate_group_count(path, wire.keys, wire.groups, "deduplicator group count")?;
        Ok(Self {
            domain: parse_domain(path, &wire.domain)?,
            entity: parse_entity(path, &wire.entity)?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            branch_fingerprint: wire.branch_fingerprint.map(BranchKeyFingerprint::new),
            branch: wire.branch,
            revision: wire.revision,
            keys: wire.keys,
            groups: wire.groups,
        })
    }
}

impl ArchiveRecord for WindowStateDescriptor {
    const KIND: RecordKind = RecordKind::WindowStateDescriptor;
    const VERSION: u16 = 1;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            rows.push(WindowRowWire {
                low_watermark_unix_nanos: row.ingested_at_low_watermark.unix_nanos(),
                high_watermark_unix_nanos: row.ingested_at_high_watermark.unix_nanos(),
            });
        }
        let mut accumulators = Vec::with_capacity(self.accumulators.len());
        for accumulator in &self.accumulators {
            accumulators.push(WindowAccumulatorWire::from(accumulator));
        }
        encode_record(
            Self::KIND,
            Self::VERSION,
            &WindowDescriptorWire {
                domain: self.domain.as_str().to_string(),
                entity: self.entity.as_str().to_string(),
                schema: *self.schema.as_digest(),
                model: *self.model.as_digest(),
                branch_fingerprint: self
                    .branch_fingerprint
                    .as_ref()
                    .map(|key| *key.fingerprint()),
                branch: self.branch.clone(),
                revision: self.revision,
                incarnation: self.incarnation,
                first_sequence: self.first_sequence,
                next_sequence: self.next_sequence,
                rows,
                groups: self.groups,
                accumulators,
            },
        )
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: WindowDescriptorWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        validate_branch(path, &wire.branch)?;
        if wire.branch.is_some() != wire.branch_fingerprint.is_some() {
            return Err(invalid(path, "window branch fingerprint"));
        }
        if wire.incarnation == 0 {
            return Err(invalid(path, "window branch incarnation"));
        }
        let rows = u64::try_from(wire.rows.len()).map_err(|_| invalid(path, "window rows"))?;
        validate_group_count(path, rows, wire.groups, "window group count")?;
        match wire.first_sequence {
            None => {
                if rows != 0 {
                    return Err(invalid(path, "window row sequence"));
                }
            }
            Some(first) => {
                let Some(end) = first.checked_add(rows) else {
                    return Err(invalid(path, "window row sequence"));
                };
                if rows == 0 || end > wire.next_sequence {
                    return Err(invalid(path, "window row sequence"));
                }
            }
        }
        let mut retained = Vec::with_capacity(wire.rows.len());
        for row in wire.rows {
            if row.low_watermark_unix_nanos > row.high_watermark_unix_nanos {
                return Err(invalid(path, "window row watermarks"));
            }
            retained.push(RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: Timestamp::from_unix_nanos(row.low_watermark_unix_nanos),
                ingested_at_high_watermark: Timestamp::from_unix_nanos(
                    row.high_watermark_unix_nanos,
                ),
            });
        }
        let mut accumulators = Vec::with_capacity(wire.accumulators.len());
        for accumulator in wire.accumulators {
            accumulators.push(WindowAccumulatorRecord::from(accumulator));
        }
        Ok(Self {
            domain: parse_domain(path, &wire.domain)?,
            entity: parse_entity(path, &wire.entity)?,
            schema: SchemaFingerprint::from_digest(wire.schema),
            model: WindowModelDigest::from_digest(wire.model),
            branch_fingerprint: wire.branch_fingerprint.map(BranchKeyFingerprint::new),
            branch: wire.branch,
            revision: wire.revision,
            incarnation: wire.incarnation,
            first_sequence: wire.first_sequence,
            next_sequence: wire.next_sequence,
            rows: retained,
            groups: wire.groups,
            accumulators,
        })
    }
}

impl From<&WindowAccumulatorRecord> for WindowAccumulatorWire {
    fn from(accumulator: &WindowAccumulatorRecord) -> Self {
        match accumulator {
            WindowAccumulatorRecord::Retained => Self::Retained,
            WindowAccumulatorRecord::LinearHistogram { delayed_removals } => {
                Self::LinearHistogram {
                    delayed_removals: delayed_removals
                        .iter()
                        .map(|removal| DelayedRemovalWire {
                            expires_at_unix_nanos: removal.expires_at.unix_nanos(),
                            bucket: removal.bucket,
                        })
                        .collect(),
                }
            }
        }
    }
}

impl From<WindowAccumulatorWire> for WindowAccumulatorRecord {
    fn from(accumulator: WindowAccumulatorWire) -> Self {
        match accumulator {
            WindowAccumulatorWire::Retained => Self::Retained,
            WindowAccumulatorWire::LinearHistogram { delayed_removals } => Self::LinearHistogram {
                delayed_removals: delayed_removals
                    .into_iter()
                    .map(|removal| DelayedHistogramRemoval {
                        expires_at: Timestamp::from_unix_nanos(removal.expires_at_unix_nanos),
                        bucket: removal.bucket,
                    })
                    .collect(),
            },
        }
    }
}

/// A state with no rows has no groups, and each group holds at least one row.
fn validate_group_count(
    path: &str,
    rows: u64,
    groups: u32,
    field: &'static str,
) -> Result<(), Report<ArchiveReadError>> {
    if (groups == 0) != (rows == 0) || u64::from(groups) > rows {
        return Err(invalid(path, field));
    }
    Ok(())
}

fn parse_domain(path: &str, raw: &str) -> Result<DomainName, Report<ArchiveReadError>> {
    DomainName::parse(raw).map_err(|_| invalid(path, "domain name"))
}

fn parse_entity(path: &str, raw: &str) -> Result<ModelName, Report<ArchiveReadError>> {
    ModelName::parse(raw).map_err(|_| invalid(path, "state entity name"))
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

    use super::*;
    use crate::archive_values::Values;

    #[test]
    fn bolero_branch_state_descriptors_preserve_every_field() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(4096)
            .for_each(|bytes| {
                let mut values = Values::new(bytes);
                let domain = values.0.name();
                for shape in 0..2 {
                    let deduplicator = values.deduplicator(&domain, shape);
                    crate::archive_properties::assert_record(&deduplicator.descriptor);
                    let window = values.window(&domain, shape);
                    crate::archive_properties::assert_record(&window.descriptor);
                }
            });
    }

    #[test]
    fn branch_state_headers_reject_wrong_kinds_and_versions_before_payload_validation() {
        for kind in [
            DeduplicatorStateDescriptor::KIND,
            WindowStateDescriptor::KIND,
        ] {
            for (tag, version) in [(kind.tag(), 2_u16), (u16::MAX, 1)] {
                let mut bytes = crate::section::RECORD_MAGIC.to_vec();
                bytes.extend_from_slice(&tag.to_le_bytes());
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.push(255);
                let error = if kind == DeduplicatorStateDescriptor::KIND {
                    DeduplicatorStateDescriptor::decode("descriptor.rkyv", &bytes).err()
                } else {
                    WindowStateDescriptor::decode("descriptor.rkyv", &bytes).err()
                };
                let Some(error) = error else {
                    panic!("a foreign {kind} header was accepted");
                };
                assert!(matches!(
                    error.current_context(),
                    ArchiveReadError::ForeignRecordKind { .. }
                        | ArchiveReadError::UnsupportedRecordVersion { .. }
                ));
            }
        }
    }

    #[test]
    fn an_empty_window_has_no_sequence_and_a_retaining_window_names_its_first_row() {
        let domain = DomainName::parse("prod").assured("the test domain is valid");
        let entity = ModelName::parse("latency_window").assured("the test entity is valid");
        let empty = WindowStateDescriptor {
            domain,
            entity,
            schema: SchemaFingerprint::from_digest([1; 32]),
            model: WindowModelDigest::from_digest([2; 32]),
            branch_fingerprint: None,
            branch: None,
            revision: 4,
            incarnation: 1,
            first_sequence: None,
            next_sequence: 9,
            rows: Vec::new(),
            groups: 0,
            accumulators: vec![WindowAccumulatorRecord::LinearHistogram {
                delayed_removals: vec![DelayedHistogramRemoval {
                    expires_at: Timestamp::from_unix_nanos(5),
                    bucket: 2,
                }],
            }],
        };
        crate::archive_properties::assert_record(&empty);
        let row = RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: Timestamp::from_unix_nanos(3),
            ingested_at_high_watermark: Timestamp::from_unix_nanos(4),
        };
        let retaining = WindowStateDescriptor {
            first_sequence: Some(7),
            rows: vec![row.clone(), row],
            groups: 1,
            ..empty.clone()
        };
        crate::archive_properties::assert_record(&retaining);
        let overflowing = WindowStateDescriptor {
            first_sequence: Some(u64::MAX),
            next_sequence: u64::MAX,
            ..retaining
        };
        crate::malformed_properties::rejects_field::<WindowStateDescriptor>(
            overflowing
                .encode()
                .assured("a malformed bounded descriptor encodes"),
            "window row sequence",
        );
    }
}
