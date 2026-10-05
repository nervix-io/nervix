//! Bounded materialized archive inputs, including empty and multi-group generations.
//!
//! Layer: test harness.
//! - **Owns.** Synthetic generation metadata, ordered identities and valid Arrow IPC payloads.
//! - **Depends on.** Current archive contracts, vocabulary generators and the external Arrow writer.
//! - **Must not know.** Runtime containers, ownership assignment or restore installation.

use arrow_array::{
    ArrayRef, BinaryArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    types::Int64Type,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainName, ModelName, RemoteRuntimeRecordMetadata, SchemaFingerprint};
use nervix_primitives::sync::StdArc;

use crate::{
    MaterializedIdentitiesRecord, MaterializedRecordIdentity, MaterializedRelayDescriptor,
    SectionContent, SectionPath, StateField, StateValue,
    archive_values::{Section, Values},
    wasm_properties::Descriptors,
};

pub(super) struct Group {
    pub identities: MaterializedIdentitiesRecord,
    pub columns: Vec<u8>,
}

pub(super) struct Materialized {
    pub descriptor: MaterializedRelayDescriptor,
    pub groups: Vec<Group>,
}

impl Materialized {
    pub fn sections(&self, sections: &mut Vec<Section>) {
        let descriptor = &self.descriptor;
        sections.push(Section::record(
            SectionPath::materialized_descriptor(&descriptor.domain, &descriptor.entity),
            descriptor,
        ));
        for group in &self.groups {
            let identities = &group.identities;
            sections.push(Section::record(
                SectionPath::materialized_identities(
                    &identities.domain,
                    &identities.entity,
                    identities.group,
                ),
                identities,
            ));
            sections.push(Section::new(
                SectionPath::materialized_columns(
                    &identities.domain,
                    &identities.entity,
                    identities.group,
                ),
                SectionContent::MaterializedColumns,
                group.columns.clone(),
            ));
        }
    }
}

impl Values<'_> {
    pub fn materialized(&mut self, domain: &DomainName, shape: u8) -> Materialized {
        let entity = ModelName::parse(match shape {
            0 => "empty_materialized",
            1 => "unbranched_materialized",
            _ => "branched_materialized",
        })
        .assured("synthetic relay names are valid");
        let count = match shape {
            0 => 0,
            1 => 1,
            _ => 1 + self.0.entropy().count(2),
        };
        let mut groups = Vec::new();
        let mut record_count = 0_u64;
        for index in 0..count {
            let rows = if shape == 1 {
                1
            } else {
                1 + self.0.entropy().count(2)
            };
            let mut identities = Vec::new();
            for row in 0..rows {
                let branch = if shape == 1 {
                    None
                } else {
                    let mut branch = Descriptors(self.0.clone())
                        .descriptor(true)
                        .branch
                        .assured("a generated branched descriptor has a key");
                    branch.push(StateField {
                        name: "zz_identity".into(),
                        value: StateValue::U8(
                            u8::try_from(index * 3 + row).assured("the bounded row identity fits"),
                        ),
                    });
                    Some(branch)
                };
                let low = self.timestamp();
                let high = self.timestamp();
                identities.push(MaterializedRecordIdentity {
                    branch,
                    watermarks: RemoteRuntimeRecordMetadata {
                        ingested_at_low_watermark: low.min(high),
                        ingested_at_high_watermark: low.max(high),
                    },
                });
            }
            record_count += u64::try_from(rows).assured("bounded row counts fit");
            groups.push(Group {
                identities: MaterializedIdentitiesRecord {
                    domain: domain.clone(),
                    entity: entity.clone(),
                    group: u32::try_from(index).assured("bounded group counts fit"),
                    identities,
                },
                columns: self.materialized_columns(rows),
            });
        }
        Materialized {
            descriptor: MaterializedRelayDescriptor {
                domain: domain.clone(),
                entity,
                schema: SchemaFingerprint::from_digest(self.digest()),
                revision: self.0.entropy().any_u64(),
                fence: self.0.entropy().any_u64(),
                branch_generation: self.0.entropy().any_u64(),
                record_count,
                groups: u32::try_from(count).assured("bounded group counts fit"),
            },
            groups,
        }
    }

    fn materialized_columns(&mut self, rows: usize) -> Vec<u8> {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("nullable", DataType::Int64, true),
            Field::new("secret", DataType::Utf8, true)
                .with_metadata([("sensitive".into(), "true".into())].into_iter().collect()),
            Field::new("bytes", DataType::Binary, true),
            Field::new("float", DataType::Float64, false),
            Field::new(
                "nested",
                DataType::List(StdArc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));
        let payloads = (0..rows).map(|_| self.payload()).collect::<Vec<_>>();
        let numbers = (0..rows)
            .map(|_| self.0.entropy().flag().then(|| self.0.entropy().any_i64()))
            .collect::<Vec<_>>();
        let strings = (0..rows)
            .map(|_| self.0.entropy().flag().then(|| self.0.string()))
            .collect::<Vec<_>>();
        let floats = (0..rows)
            .map(|_| f64::from_bits(self.0.entropy().any_u64()))
            .collect::<Vec<_>>();
        let nested = (0..rows)
            .map(|_| {
                self.0
                    .entropy()
                    .flag()
                    .then(|| vec![Some(self.0.entropy().any_i64()), None])
            })
            .collect::<Vec<_>>();
        let columns: Vec<ArrayRef> = vec![
            StdArc::new(Int64Array::from(numbers)),
            StdArc::new(StringArray::from(strings)),
            StdArc::new(BinaryArray::from_iter_values(
                payloads.iter().map(Vec::as_slice),
            )),
            StdArc::new(Float64Array::from(floats)),
            StdArc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(nested)),
        ];
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
}
