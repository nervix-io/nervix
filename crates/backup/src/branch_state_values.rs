//! Bounded deduplicator and window archive inputs, including empty, unbranched and multi-group
//! states.
//!
//! Layer: test harness.
//! - **Owns.** Synthetic keyspace and window descriptors and valid row-aligned Arrow IPC groups.
//! - **Depends on.** Current archive contracts, vocabulary generators and the external Arrow writer.
//! - **Must not know.** Runtime keyspaces or windows, ownership assignment or restore installation.

use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchKeyFingerprint, DomainName, ModelName, RemoteRuntimeRecordMetadata, SchemaFingerprint,
    Timestamp, WindowModelDigest,
};
use nervix_primitives::sync::StdArc;

use crate::{
    DeduplicatorStateDescriptor, DelayedHistogramRemoval, SectionContent, SectionPath, StateField,
    StateValue, WindowAccumulatorRecord, WindowStateDescriptor,
    archive_values::{Section, Values},
    wasm_properties::Descriptors,
};

pub(super) struct Deduplicator {
    pub descriptor: DeduplicatorStateDescriptor,
    pub groups: Vec<Vec<u8>>,
}

pub(super) struct WindowGroup {
    pub input: Vec<u8>,
    pub arguments: Vec<u8>,
}

pub(super) struct Window {
    pub descriptor: WindowStateDescriptor,
    pub groups: Vec<WindowGroup>,
}

impl Deduplicator {
    pub fn sections(&self, sections: &mut Vec<Section>) {
        let descriptor = &self.descriptor;
        let branch = descriptor.branch_fingerprint.as_ref();
        sections.push(Section::record(
            SectionPath::deduplicator_descriptor(&descriptor.domain, &descriptor.entity, branch),
            descriptor,
        ));
        for (index, keys) in self.groups.iter().enumerate() {
            let group = u32::try_from(index).assured("bounded group counts fit");
            sections.push(Section::new(
                SectionPath::deduplicator_keys(
                    &descriptor.domain,
                    &descriptor.entity,
                    branch,
                    group,
                ),
                SectionContent::DeduplicatorKeys,
                keys.clone(),
            ));
        }
    }
}

impl Window {
    pub fn sections(&self, sections: &mut Vec<Section>) {
        let descriptor = &self.descriptor;
        let branch = descriptor.branch_fingerprint.as_ref();
        sections.push(Section::record(
            SectionPath::window_descriptor(&descriptor.domain, &descriptor.entity, branch),
            descriptor,
        ));
        for (index, group) in self.groups.iter().enumerate() {
            let number = u32::try_from(index).assured("bounded group counts fit");
            // Arguments sort before input within a group, which the reader accepts either way.
            sections.push(Section::new(
                SectionPath::window_argument_columns(
                    &descriptor.domain,
                    &descriptor.entity,
                    branch,
                    number,
                ),
                SectionContent::WindowArgumentColumns,
                group.arguments.clone(),
            ));
            sections.push(Section::new(
                SectionPath::window_input_rows(
                    &descriptor.domain,
                    &descriptor.entity,
                    branch,
                    number,
                ),
                SectionContent::WindowInputRows,
                group.input.clone(),
            ));
        }
    }
}

/// One Arrow IPC stream holding `columns` under `schema`.
fn arrow_stream(schema: Schema, columns: Vec<ArrayRef>) -> Vec<u8> {
    let schema = StdArc::new(schema);
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .assured("bounded synthetic columns match their exact schema");
    let mut bytes = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut bytes, &schema).assured("the synthetic Arrow stream opens");
    writer
        .write(&batch)
        .assured("the bounded Arrow batch writes");
    writer.finish().assured("the Arrow stream ends");
    bytes
}

impl Values<'_> {
    /// A branch key with a distinguishing field, or none for an unbranched state.
    fn branch_identity(
        &mut self,
        branched: bool,
        identity: u8,
    ) -> (Option<BranchKeyFingerprint>, Option<Vec<StateField>>) {
        if !branched {
            return (None, None);
        }
        let mut branch = Descriptors(self.0.clone())
            .descriptor(true)
            .branch
            .assured("a generated branched descriptor has a key");
        branch.push(StateField {
            name: "zz_identity".into(),
            value: StateValue::U8(identity),
        });
        (
            Some(BranchKeyFingerprint::new([identity; 32])),
            Some(branch),
        )
    }

    fn watermarks(&mut self) -> RemoteRuntimeRecordMetadata {
        let low = self.timestamp();
        let high = self.timestamp();
        RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: low.min(high),
            ingested_at_high_watermark: low.max(high),
        }
    }

    /// An empty unbranched keyspace for shape 0, and a branched keyspace in up to three groups
    /// otherwise.
    pub fn deduplicator(&mut self, domain: &DomainName, shape: u8) -> Deduplicator {
        let branched = shape != 0;
        let entity = ModelName::parse(if branched {
            "branched_deduplicator"
        } else {
            "unbranched_deduplicator"
        })
        .assured("synthetic deduplicator names are valid");
        let (branch_fingerprint, branch) = self.branch_identity(branched, 3);
        let count = if branched {
            1 + self.0.entropy().count(2)
        } else {
            0
        };
        let mut groups = Vec::new();
        let mut keys = 0_u64;
        for _ in 0..count {
            let rows = 1 + self.0.entropy().count(3);
            keys = keys
                .checked_add(u64::try_from(rows).assured("bounded key counts fit"))
                .assured("a bounded keyspace holds fewer keys than u64 counts");
            let schema = Schema::new(vec![
                Field::new("key_0", DataType::Utf8, true),
                Field::new("key_1", DataType::Int64, true),
                Field::new(
                    "seen_at",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    false,
                ),
            ]);
            let texts = (0..rows)
                .map(|_| self.0.entropy().flag().then(|| self.0.string()))
                .collect::<Vec<_>>();
            let numbers = (0..rows)
                .map(|_| self.0.entropy().flag().then(|| self.0.entropy().any_i64()))
                .collect::<Vec<_>>();
            let seen = (0..rows)
                .map(|_| self.timestamp().unix_nanos())
                .collect::<Vec<_>>();
            groups.push(arrow_stream(
                schema,
                vec![
                    StdArc::new(StringArray::from(texts)),
                    StdArc::new(Int64Array::from(numbers)),
                    StdArc::new(TimestampNanosecondArray::from(seen).with_timezone("+00:00")),
                ],
            ));
        }
        Deduplicator {
            descriptor: DeduplicatorStateDescriptor {
                domain: domain.clone(),
                entity,
                schema: SchemaFingerprint::from_digest(self.digest()),
                branch_fingerprint,
                branch,
                revision: self.0.entropy().any_u64(),
                keys,
                groups: u32::try_from(count).assured("bounded group counts fit"),
            },
            groups,
        }
    }

    /// An unbranched window retaining no rows for shape 0, and a branched window with up to three
    /// row groups and delayed histogram removals otherwise.
    pub fn window(&mut self, domain: &DomainName, shape: u8) -> Window {
        let branched = shape != 0;
        let entity = ModelName::parse(if branched {
            "branched_window"
        } else {
            "unbranched_window"
        })
        .assured("synthetic window names are valid");
        let (branch_fingerprint, branch) = self.branch_identity(branched, 5);
        let count = if branched {
            1 + self.0.entropy().count(2)
        } else {
            0
        };
        let mut groups = Vec::new();
        let mut rows = Vec::new();
        for _ in 0..count {
            let group_rows = 1 + self.0.entropy().count(3);
            for _ in 0..group_rows {
                let watermarks = self.watermarks();
                rows.push(watermarks);
            }
            let latencies = (0..group_rows)
                .map(|_| self.0.entropy().any_i64())
                .collect::<Vec<_>>();
            let tenants = (0..group_rows)
                .map(|_| self.0.entropy().flag().then(|| self.0.string()))
                .collect::<Vec<_>>();
            let input = arrow_stream(
                Schema::new(vec![
                    Field::new("latency", DataType::Int64, false),
                    Field::new("tenant", DataType::Utf8, true),
                ]),
                vec![
                    StdArc::new(Int64Array::from(latencies)),
                    StdArc::new(StringArray::from(tenants)),
                ],
            );
            let measures = (0..group_rows)
                .map(|_| {
                    self.0
                        .entropy()
                        .flag()
                        .then(|| f64::from_bits(self.0.entropy().any_u64()))
                })
                .collect::<Vec<_>>();
            let counts = (0..group_rows)
                .map(|_| self.0.entropy().flag().then(|| self.0.entropy().any_i64()))
                .collect::<Vec<_>>();
            let arguments = arrow_stream(
                Schema::new(vec![
                    Field::new("argument_0", DataType::Float64, true),
                    Field::new("argument_1", DataType::Int64, true),
                ]),
                vec![
                    StdArc::new(Float64Array::from(measures)),
                    StdArc::new(Int64Array::from(counts)),
                ],
            );
            groups.push(WindowGroup { input, arguments });
        }
        let retained = u64::try_from(rows.len()).assured("bounded row counts fit");
        let first_sequence = if rows.is_empty() {
            None
        } else {
            let latest = u64::MAX
                .checked_sub(retained)
                .assured("a bounded window retains fewer rows than u64 counts");
            Some(self.0.entropy().boundary_biased(0..=latest))
        };
        let next_sequence = match first_sequence {
            Some(first) => first
                .checked_add(retained)
                .assured("the first sequence leaves room for every retained row"),
            None => self.0.entropy().any_u64(),
        };
        let removals = (0..self.0.entropy().count(3))
            .map(|_| DelayedHistogramRemoval {
                expires_at: Timestamp::from_unix_nanos(self.0.entropy().any_i64()),
                bucket: self.0.entropy().any_u64(),
            })
            .collect();
        Window {
            descriptor: WindowStateDescriptor {
                domain: domain.clone(),
                entity,
                schema: SchemaFingerprint::from_digest(self.digest()),
                model: WindowModelDigest::from_digest(self.digest()),
                branch_fingerprint,
                branch,
                revision: self.0.entropy().any_u64(),
                incarnation: self.0.positive_u64().get(),
                first_sequence,
                next_sequence,
                rows,
                groups: u32::try_from(count).assured("bounded group counts fit"),
                accumulators: vec![
                    WindowAccumulatorRecord::Retained,
                    WindowAccumulatorRecord::LinearHistogram {
                        delayed_removals: removals,
                    },
                ],
            },
            groups,
        }
    }
}
